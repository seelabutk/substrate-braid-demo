"""

"""

from __future__ import annotations
from .. import Service, Job, Result, Host, Cluster, Channel
import numpy as np
from collections import defaultdict
from vaas_streamlines import StreamlineService
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import cycle
from typing import List, Dict, Tuple, Optional, Union
from dataclasses import dataclass
import json


__all__ = ['streamline']


streamline: Service = Service('streamline')


@streamline.register
@dataclass(frozen=True)
class Streamline0Result(Result):
	seq: List[int]
	index: List[int]
	points: List[np.array]
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		seq = data['seq']
		index = data['index']
		points = [np.array(x).reshape((-1, 3)) for x in data['points']]
		return cls(seq, index, points)
	
	def dumps(self):
		data = {}
		data['seq'] = self.seq
		data['index'] = self.index
		data['points'] = [x.reshape(-1).tolist() for x in self.points]
		return json.dumps(data)


@streamline.register
@dataclass(frozen=True)
class Streamline0Job(Job):
	partitions: int
	hostid: int
	hosts: List[Host]
	seeds: List[float, float, float]
	seq: int
	index: List[int]
	lengths: List[int]
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		partitions = data['partitions']
		hostid = data['hostid']
		try:
			hosts = [Host.loads(x) for x in data['hosts']]
		except KeyError:
			hosts = None
		seeds = data['seeds']
		_nseeds = len(seeds) // 3
		try:
			seq = data['seq']
		except KeyError:
			seq = 0
		try:
			index = data['index']
		except KeyError:
			index = [i for i in range(_nseeds)]
		try:
			lengths = data['lengths']
		except KeyError:
			lengths = [10000 for _ in range(_nseeds)]
		return cls(partitions, hostid, hosts, seeds, seq, index, lengths)
	
	def dumps(self):
		data = {}
		data['partitions'] = self.partitions
		data['hostid'] = self.hostid
		data['hosts'] = [x.dumps() for x in self.hosts]
		data['seeds'] = self.seeds
		data['seq'] = self.seq
		data['index'] = self.index
		data['lengths'] = self.lengths
		return json.dumps(data)
	
	def __call__(self, channel: Channel, cluster: Cluster) -> Streamline0Result:
		s = StreamlineService()
		s.filename = '/data/interp9000.raw'
		s.dimensions = (490, 490, 280)
		s.maxSteps = np.minimum(100, np.array(self.lengths, dtype='uint32'))
		s.seeds = np.array(self.seeds, dtype='float32')
		s.hostid = self.hostid
		s.nhosts = self.partitions
		res = s()
		
		to_run = defaultdict(list)
		new_hostid = defaultdict(list)
		new_index = defaultdict(list)
		new_lengths = defaultdict(list)
		new_seeds = defaultdict(list)
		it = zip(res.finished, self.lengths, res.forwarded, res.streamlines)
		for i, (fin, exp, forw, points) in enumerate(it):
			#print(fin, self.hostid, forw, forw == self.hostid)
			is_done = False
			stopped_early = len(points) == 300 and exp > 100
			forward_to_self = forw == self.hostid
			terminated = len(points) == 1
			if fin and not stopped_early:
				is_done = True
			if stopped_early and forward_to_self:
				is_done = False
			if terminated:
				is_done = True
			if not is_done:
				forw = int(forw)
				exp = int(exp)
				new_hostid[forw].append(forw)
				new_index[forw].append(self.index[i])
				new_lengths[forw].append(exp - len(points))
				new_seeds[forw].extend(points[-1].tolist())
		
		#print('nhost', new_hostid)
		#print('new_seq', new_seq)
		#print('nindex', new_index)
		#print('nlen', new_lengths)
		#print('nseed', new_seeds)
		#print('fseq', fin_seq)
		#print('find', fin_index)
		#print('fpts', fin_points)
		
		hosts = self.hosts
		if hosts is None:
			hosts = cluster.hosts
		
		executor = ThreadPoolExecutor(len(new_hostid)) if new_hostid else None
		futures = []
		for hostid in new_hostid:
			if not (0 <= hostid < self.partitions):
				continue
			#assert 0 <= hostid < self.partitions, f'{hostid}'
			job = Streamline0Job(
				self.partitions,
				hostid,
				hosts,
				new_seeds[hostid],
				self.seq + 1,
				new_index[hostid],
				new_lengths[hostid],
			)
			host = hosts[hostid % len(hosts)]
			future = executor.submit(host.submit, channel, streamline, job)
			futures.append(future)
		
		result = Streamline0Result(
			self.seq,
			self.index,
			res.streamlines,
		)
		channel.put(result.dumps().encode('utf-8'))
		
		done, _ = wait(futures)
		for future in done:
			future.result()
