"""

"""

from __future__ import annotations
from .. import Service, Job, Result, Host, Cluster, Channel, Region
from vaas_ncar import init as ncar_init, trace as ncar_trace, load_metadata as ncar_load_metadata
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union
from dataclasses import dataclass
from collections import defaultdict
import json
from concurrent.futures import ThreadPoolExecutor, wait
from random import sample
from sys import stderr
from os import environ

import eliot


__all__ = ['ncar']


ncar: Service = Service('ncar')


_g_domain = Region(
	lo=(-90.0, 0.0, 2.0, 0.0),
	hi=(90.0, 360.0, 1000.0, 5053194.0),
)
_g_colls = None
_g_hosts = None
_g_regions = None  # w/o ghost regions
_g_domains = None  # w/ ghost regions


class NotMyDataCollection:
	pass


def init(cluster: Cluster):
	with eliot.start_action(action_type='initialize ncar service') as action:
		root = Path(environ.get('VCI_ROOT', '/mnt/seenas2/data/climate/gen'))
		pattern = environ.get('VCI_PATTERN', 'UGRD-ghost-144x73-*of*_*of*_*of*_*of*.dat')
		action.log('looking up ugrds', root=root, pattern=pattern)

		try:
			ugrds = list(root.glob(pattern))
			action.log('found this many ugrds', n=len(ugrds), first=ugrds[0] if len(ugrds) > 0 else None)
			assert len(ugrds) > 0, f"wtf: no ugrds found, root = {root!r}, pattern = {pattern!r}"
		except AssertionError:
			print(f'random selection of files in {root!r}:', file=stderr)
			paths = list(root.iterdir())
			for path in sample(paths, k=20):
				print(f'  {path!r}', file=stderr)
			raise

		hosts, parts = cluster.assign_partitions_ncar(ugrds)
		hosts: List[Host]
		parts: List[Tuple[Path[UGRD], Path[VGRD], Path[VVEL]]]

		action.log('after assigning partitions', nhosts=len(hosts), nparts=len(parts))
		assert len(hosts) == len(parts), "wtf: number of hosts and partitions is not the same"

		global _g_hosts
		_g_hosts = hosts

		regions = []
		domains = []
		colls = []
		for host, (ugrd, vgrd, vvel) in zip(hosts, parts):
			with eliot.start_action(action_type='looking at assignment', host=host, ugrd=ugrd, vgrd=vgrd) as inner_action:
				ghost = 4
				lo = (
					ncar_load_metadata(str(ugrd), ghost, 0, 3),
					ncar_load_metadata(str(ugrd), ghost, 0, 2),
					ncar_load_metadata(str(ugrd), ghost, 0, 1),
					ncar_load_metadata(str(ugrd), ghost, 0, 0),
				)
				hi = (
					ncar_load_metadata(str(ugrd), ghost, 1, 3),
					ncar_load_metadata(str(ugrd), ghost, 1, 2),
					ncar_load_metadata(str(ugrd), ghost, 1, 1),
					ncar_load_metadata(str(ugrd), ghost, 1, 0),
				)

				region = Region(lo, hi)

				ghost = 0
				lo = (
					ncar_load_metadata(str(ugrd), ghost, 0, 3),
					ncar_load_metadata(str(ugrd), ghost, 0, 2),
					ncar_load_metadata(str(ugrd), ghost, 0, 1),
					ncar_load_metadata(str(ugrd), ghost, 0, 0),
				)
				hi = (
					ncar_load_metadata(str(ugrd), ghost, 1, 3),
					ncar_load_metadata(str(ugrd), ghost, 1, 2),
					ncar_load_metadata(str(ugrd), ghost, 1, 1),
					ncar_load_metadata(str(ugrd), ghost, 1, 0),
				)

				domain = Region(lo, hi)

				inner_action.log('calculated region', region=region, domain=domain)

				if host is cluster.myself:
					inner_action.log('this one is mine')
					coll = ncar_init(str(ugrd), str(vgrd), str(vvel))
				else:
					inner_action.log('this one is not mine')
					coll = NotMyDataCollection()

				colls.append(coll)
				regions.append(region)
				domains.append(domain)
		
		global _g_colls
		_g_colls = colls

		global _g_regions
		_g_regions = regions

		global _g_domains
		_g_domains = domains

		print(f'h: {_g_hosts!r}')
		print(f'r: {_g_regions!r}')
		print(f'c: {_g_domains!r}')
		print(f'c: {_g_colls!r}')


@ncar.register
@dataclass(frozen=True)
class Ncar0Result(Result):
	points: List[float]
	nseeds: int
	steps: int
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		points = data['points']
		nseeds = data['nseeds']
		steps = data['steps']
		return cls(points, nseeds, steps)
	
	def dumps(self):
		data = {}
		data['points'] = self.points
		data['nseeds'] = self.nseeds
		data['steps'] = self.steps
		return json.dumps(data)


@ncar.register
@dataclass(frozen=True)
class Ncar0Job(Job):
	seeds: List[float]
	steps: int
	region: Region
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		seeds = data['seeds']
		steps = data['steps']
		try:
			region = Region.loads(data['region'])
		except KeyError:
			region = _g_domain
		return cls(seeds, steps, region)
	
	def dumps(self):
		data = {}
		data['seeds'] = self.seeds
		data['steps'] = self.steps
		data['region'] = self.region.dumps()
		return json.dumps(data)
	
	def __call__(self, channel: Channel, cluster: Cluster) -> Ncar0Result:
		with eliot.start_action(action_type='do ncar particle tracing', steps=self.steps, region=self.region) as action:
			seeds = self.seeds
			steps = self.steps
			if steps <= 0:
				return

			action.log('Received seeds', nseeds=len(seeds)//4)

			it = iter(seeds)
			assigns: Dict[HostId, List[Point]] = defaultdict(list)
			for seed in zip(it, it, it, it):
				for i, (host, region, coll) in enumerate(zip(_g_hosts, _g_regions, _g_colls)):
					if seed in region:
						#print(f'<Seed {seed}> assigned to {region}')
						assigns[i].append(seed)
						break
				else:
					action.log('Could not find an appropriate region', seed=seed, regions=regions)
					print(f'Could not find a region for <Seed {seed}>')

			action.log('Assigned seeds to hosts', nhosts_assigned_to=len(assigns))

			sorted_assigns = sorted(assigns.items(), key=lambda x: x[1] is cluster.myself)
			
			executor = ThreadPoolExecutor(max_workers=16)
			futures = []
			for i, seeds in sorted_assigns:
				seeds = [float(y) for x in seeds for y in x]

				host = _g_hosts[i]
				region = _g_regions[i]
				coll = _g_colls[i]
				domain = _g_domains[i]

				if host is cluster.myself:
					action.log('I am running the kernel', seeds=seeds, steps=steps, domain=domain)
					#print(f'<Host {host.netloc}> is myself')

					to_trace = min(int(environ.get('VCI_MAXSTEPS', 10000)), steps)
					action.log('to trace', to_trace=to_trace)

					points, lengths = ncar_trace(coll, seeds, to_trace, domain.lo, domain.hi)

					# Before: points is a flat list of size 4*nseeds*nsteps
					points = [
						points[4*i*steps:4*(i*steps+l)]
						for i, l in enumerate(lengths)
					]
					# After: points is a list of lists of points, where the inner list is
					# exactly the size necessary to hold the actually calculated streamlines
					#
					# points = [ [lat, lng, prs, sec, ...], [lat, lng, prs, sec, ...] ]
					
					result = Ncar0Result(
						points,
						len(self.seeds) // 4,
						self.steps,
					)
					channel.put(result.dumps().encode('utf-8'))

					seeds_and_steps_remaining: List[Tuple[Point, int]] = []
					for line, length in zip(points, lengths):
						if length == 0:
							continue
						elif length == 1:
							action.log("I think this seed will stall, so let's skip it")
							continue

						seed = tuple(line[-4:])
						nstepsleft = self.steps - length
						seeds_and_steps_remaining.append((seed, nstepsleft))
					
					batches: Dict[int, List[Tuple[Point, int]]] = defaultdict(list)
					for seed, nstepsleft in seeds_and_steps_remaining:
						for i, (region, domain) in enumerate(zip(_g_regions, _g_domains)):
							if seed in region:
								batches[i].append((seed, nstepsleft))
								break
					
					tasks: List[Tuple[Host, Job]] = []
					for i, seeds_and_nstepsleft in batches.items():
						host = _g_hosts[i]
						region = _g_regions[i]

						nstepsleft = max(nstepsleft for _, nstepsleft in seeds_and_nstepsleft)
						seeds = [x for seeds, _ in seeds_and_nstepsleft for x in seeds]  # flatten
						
						job = Ncar0Job(seeds, nstepsleft, region)
						tasks.append((host, job))

					if len(tasks) == 0:
						continue

					action.log('After running the kernel, I need to reforward some seeds', ntasks=len(tasks))

					for host, job in tasks:
						# this introduces an "eliot:remote_task" action
						target = eliot.preserve_context(host.submit)
						future = executor.submit(target, channel, ncar, job)
						futures.append(future)
					
				else:
					#print(f'<Host {host.netloc}> is not myself')
					action.log('I am forwarding some seeds', host=host, seeds=seeds)

					job = Ncar0Job(seeds, steps, region)
					target = eliot.preserve_context(host.submit)
					future = executor.submit(target, channel, ncar, job)
					futures.append(future)

			done, _ = wait(futures)
			for future in done:
				future.result()
