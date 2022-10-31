#!/usr/bin/env python3.7
"""

"""

from __future__ import annotations
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import gzip
from importlib import import_module
from dataclasses import dataclass, field
from socket import gethostname
import json
import requests
from typing import List, Tuple, ClassVar
from dns.resolver import Resolver
from time import sleep
from threading import Thread, Lock
from requests.compat import urlunparse
from queue import Queue, Empty
from uuid import uuid4
from psutil import cpu_percent
import eliot
from itertools import product
from os import environ
from .partition import assign_with_redundancy


_g_cluster: Cluster = None
_g_channels: Channels = None
_g_port: int = None


class JSONEncoder(eliot.json.EliotJSONEncoder):
	def default(self, obj):
		try:
			return super().default(obj)
		except:
			return repr(obj)


def make_url(scheme=None, netloc=None, path=None, params=None, query=None, fragment=None):
	if scheme is None: scheme = 'http'
	if netloc is None: raise ValueError
	if path is None: path = ''
	if params is None: params = ''
	if query is None: query = ''
	if fragment is None: fragment = ''
	return urlunparse((scheme, netloc, path, params, query, fragment))


def make_request(name: str, host: Host, **attrs) -> Dict[str, Any]:
	kwargs = {}
	if name in ('broadcast-host', 'list-hosts'):
		path = '/cluster/host/'
		kwargs['method'] = 'POST'
		kwargs['data'] = attrs['data']
	elif name == 'submit-job':
		path = '/service/job/' + attrs['service'].name
		kwargs['method'] = 'POST'
		kwargs['headers'] = { 'X-VaaS-Channel': attrs['channel'].dumps() }
		kwargs['data'] = attrs['data']
	elif name == 'channel-put':
		path = '/channel/' + attrs['id']
		kwargs['method'] = 'POST'
		kwargs['data'] = attrs['data']
	else:
		raise NotImplementedError
	kwargs['url'] = make_url(netloc=host.netloc, path=path)
	try:
		data = kwargs['data']
	except KeyError:
		pass
	else:
		headers = kwargs.get('headers', {})
		headers['Content-Encoding'] = 'gzip'
		kwargs['data'] = gzip.compress(data)
		kwargs['headers'] = headers
	headers['X-Vaas-Eliot-Task'] = eliot.current_action().serialize_task_id()
	return kwargs

@dataclass
class Channel:
	host: Host
	id: str
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		host = Host.loads(data['host'])
		id = data['id']
		return cls(host, id)
	
	def dumps(self):
		data = {}
		data['host'] = self.host.dumps()
		data['id'] = self.id
		return json.dumps(data)
	
	def close(self):
		raise NotImplementedError
	
	def __enter__(self):
		return self
	
	def __exit__(self, type, value, tb):
		self.close()


@dataclass
class LocalChannel(Channel):
	is_remote: ClassVar[bool] = False
	_messages: Optional[Queue] = field(default_factory=Queue)
	
	def put(self, value) -> bool:
		self._messages.put(value)
		return True
	
	def get(self, timeout=None) -> bytes:
		"""Throws queue.Empty if no message in time"""
		return self._messages.get(timeout=timeout)
	
	def close(self, *, channels=None):
		if channels is None:
			channels = _g_channels
		channels.finish(self.id)


@dataclass
class RemoteChannel(Channel):
	is_remote: ClassVar[bool] = True

	def put(self, value) -> bool:
		return self.host.channel_put(self.id, value)
	
	def get(self):
		raise ValueError("Can't get a remote channel")
	
	def close(self):
		pass
	
	def heartbeat(self) -> bool:
		return self.put(b'heartbeat\r\n')


@dataclass
class Channels:
	channels: Dict[str, Channel]
	
	def __getitem__(self, key):
		return self.channels[key]
	
	def __contains__(self, key):
		return key in self.channels
	
	@classmethod
	def new(cls):
		channels = {}
		return cls(channels)
	
	def make_local(self, id: str=None, *, cluster=None):
		if cluster is None:
			cluster = _g_cluster

		if id is None:
			id = str(uuid4())
		channel = LocalChannel(cluster.myself, id)
		self.channels[id] = channel
		return channel
	
	def finish(self, id: str):
		try:
			del self.channels[id]
		except KeyError:
			print(f'Already finished channel {id}')
		else:
			print(f'Finished channel {id}')


class Service:
	_lookup: dict = {}
	
	def __init__(self, name):
		self.name = name
		self.result_class = None
		self.job_class = None
		Service._lookup[name] = self
	
	def register(self, clazz):
		name = clazz.__name__
		if 'Job' in name:  # lol
			self.job_class = clazz
		elif 'Result' in name:  # lol
			self.result_class = clazz
		else:
			raise NotImplementedError
		return clazz
	
	@classmethod
	def lookup(cls, name):
		return cls._lookup[name]
	
	@classmethod
	def get_reference(cls, name):
		try:
			service = cls._lookup[name]
		except KeyError:
			package = name
			if '/' in package:
				package, _ = package.split('/')
			module = import_module(f'vci.services.{package}', package='vci.services')
			if hasattr(module, 'init'):
				module.init(_g_cluster)
			service = cls._lookup[name]
		return service


@dataclass(frozen=True)
class Result:
	@classmethod
	def loads(cls, data: str):
		raise NotImplementedError
	
	def dumps(self) -> str:
		raise NotImplementedError


@dataclass(frozen=True)
class Job:
	@classmethod
	def loads(cls, data: str):
		raise NotImplementedError
	
	def dumps(self) -> str:
		raise NotImplementedError
	
	def __call__(self, channel: Channel, cluster: Cluster) -> Result:
		raise NotImplementedError



@dataclass(frozen=True)
class Region:
	lo: Tuple[float, float, float, float]
	hi: Tuple[float, float, float, float]
	
	def __post_init__(self):
		assert self.lo <= self.hi
		assert isinstance(self.lo, tuple)
		assert isinstance(self.hi, tuple)
	
	@classmethod
	def loads(cls, data: str) -> Region:
		data = json.loads(data)
		lo = tuple(data['lo'])
		hi = tuple(data['hi'])
		return cls(lo, hi)
	
	def dumps(self) -> str:
		data = {}
		data['lo'] = self.lo
		data['hi'] = self.hi
		return json.dumps(data)
	
	def __add__(self, other: float) -> Region:
		lo = [x - other for x in self.lo]
		hi = [x + other for x in self.hi]
		return Region(lo, hi)
	
	def __contains__(self, point: Point) -> bool:
		return all(lo <= p <= hi for lo, p, hi in zip(self.lo, point, self.hi))
	
	def __floordiv__(self, n: int) -> List[Region]:
		r = range(n)
		regions = []
		for index in product(r, r, r, r):
			lo = tuple(x+(y-x)/n*(i+0) for i, x, y in zip(index, self.lo, self.hi))
			hi = tuple(x+(y-x)/n*(i+1) for i, x, y in zip(index, self.lo, self.hi))
			region = Region(lo, hi)
			regions.append(region)

		return regions
	


@dataclass(order=True)
class Host:
	netloc: str
	cpu: float = field(compare=False, default=0.0)
	regions: List[Region] = field(compare=False, default_factory=list)
	
	@classmethod
	def from_system(cls):
		import random
		import subprocess

		hostname = gethostname()
		netloc = subprocess.check_output(['hostname', '-I']).split()[0].decode('utf-8')
		cpu = random.random() * 10
		regions = []
		return cls(f'{netloc}:{_g_port}', cpu, regions)
	
	@classmethod
	def loads(cls, data: str):
		data = json.loads(data)
		netloc = data['netloc']
		cpu = data['cpu']
		regions = [Region.loads(x) for x in data['regions']]
		return cls(netloc, cpu)
	
	def dumps(self) -> str:
		data = {}
		data['netloc'] = self.netloc
		data['cpu'] = self.cpu
		data['regions'] = [x.dumps() for x in self.regions]
		return json.dumps(data)

	def sync(self) -> None:
		with requests.get(f'http://{self.netloc}/host/sync/') as r:
			data = r.json()
		self.cpu = data['cpu']
		self.regions = [Region.loads(x) for x in data['regions']]

	def update(self) -> None:
		self.cpu = cpu_percent()

	def run_updater_thread(self) -> None:
		def inner():
			while True:
				try:
					self.update()
				except:
					import traceback; traceback.print_exc()
				finally:
					sleep(0.25)

		Thread(target=inner).start()
	
	def broadcast_host(self, other: 'Host'):
		"""Broadcast other to self"""
		data = other.dumps().encode('utf-8')
		kwargs = make_request('broadcast-host', host=self, data=data)
		with requests.request(**kwargs) as r:
			pass
	
	#@eliot.log_call(action_type='Host.submit', include_args=())
	def submit(self, channel: Channel, service: 'Service', job: 'Job') -> 'Result':
		with eliot.start_action(action_type='submit job to host', job_class=job.__class__.__name__) as action:
			data = job.dumps().encode('utf-8')
			kwargs = make_request('submit-job', host=self, channel=channel, service=service, data=data)
			action.log('make_request returned', url=kwargs['url'])
			with requests.request(**kwargs) as r:
				result = r.content # service.result_class.loads(r.content)
			action.add_success_fields(result=result)

		return result
	
	def channel_put(self, id: str, value: bytes) -> bool:
		with eliot.start_action(action_type='write to channel', id=id, heartbeat=value==b'heartbeat\r\n') as action:
			data = value
			kwargs = make_request('channel-put', host=self, id=id, data=data)
			with requests.request(**kwargs) as r:
				result = r.ok
			action.add_success_fields(result=result)

		return result


@dataclass
class Cluster:
	myself: Host
	hosts: List[Host]
	lock: Lock = field(default_factory=Lock)
	
	@property
	def other_hosts(self):
		return [host for host in self.hosts if host != self.myself]
	
	@classmethod
	def from_myself(cls, myself: Host):
		hosts = [myself]
		return cls(myself, hosts)
	
	@classmethod
	def loads(cls, data: str):
		data = json.loads(data)
		myself = Host.loads(data['myself'])
		hosts = [Host.loads(x) for x in data['hosts']]
		return cls(myself, hosts)
	
	def add_docker_hosts(self, service_name):
		resolver = Resolver()
		for answer in resolver.query('tasks.%s.' % (service_name,)):
			host = Host(f'{answer}:{_g_port}')
			modified = self.add(host)
			if False and modified:
				self.broadcast_host(host)
				host.broadcast_host(self.myself)
	
	def run_updater_thread(self, service_name):
		def inner():
			while True:
				sleep(5)
				try:
					self.add_docker_hosts(service_name)
				except:
					import traceback; traceback.print_exc()

				for host in self.other_hosts:
					try:
						host.sync()
					except:
						import traceback; traceback.print_exc()
		
		Thread(target=inner).start()
	
	def dumps(self) -> str:
		data = {}
		data['myself'] = self.myself.dumps()
		data['hosts'] = [x.dumps() for x in self.hosts]
		return json.dumps(data)
	
	def add(self, host: Host) -> bool:
		modified = False
		with self.lock:
			if host != self.myself and host not in self.hosts:
				self.hosts.append(host)
				self.hosts.sort()  # this really needs a lock
				modified = True
		return modified
	
	def merge_inplace(self, other: 'Cluster') -> Tuple[bool, List[Host]]:
		modified = False
		new_hosts = []
		with other.lock:
			for host in other.hosts:
				if self.add(host):
					new_hosts.append(host)
					modified = True
		new_hosts.sort()
		return modified, new_hosts
	
	def broadcast_host(self, other: 'Host'):
		for host in self.other_hosts:
			host.broadcast_host(other)
	
	def broadcast_job(self, service: 'Service', job: 'Job'):
		for host in self.other_hosts:
			host.submit(service, job)
	
	def assign_partitions_ncar(self, ugrds: List[Path[UGRD]]) -> Tuple[List[Host], List[Path[UGRD], Path[VGRD], Path[VVEL]]]:
		hosts = sorted(self.hosts)

		parts: List[Path[UGRD], Path[VGRD], Path[VVEL]] = []
		for ugrd in ugrds:
			root = ugrd.parent
			name = ugrd.name
			vgrd = root / name.replace('UGRD', 'VGRD')
			vvel = root / name.replace('UGRD', 'VVEL')
			parts.append((ugrd, vgrd, vvel))
		
		redundancy = int(environ.get('VCI_REDUNDANCY', '1'))
		assigns: Dict[HostId, List[PartId]] = assign_with_redundancy(len(hosts), len(parts), redundancy)

		ret_hosts = []
		ret_parts = []
		for hostid, partids in assigns.items():
			host = hosts[hostid]
			for partid in partids:
				part = parts[partid]
				ret_hosts.append(host)
				ret_parts.append(part)
		
		return ret_hosts, ret_parts
	
	def assign_partitions(self, domain: Region) -> Tuple[List[Host], List[Region]]:
		hosts = sorted(self.hosts)
		for i in range(1, 10):
			n_partitions = i ** 4
			if n_partitions >= len(hosts):
				break
		else:
			raise NotImplementedError
		
		regions = domain // i
		hosts = sorted(self.hosts)
		ret_hosts = []
		ret_regions = []
		for i, region in enumerate(regions):
			host = hosts[i % len(hosts)]
			ret_hosts.append(host)
			ret_regions.append(region)
		
		return ret_hosts, ret_regions


def continue_task_from_header(action_type=None, **fields):
	from functools import wraps, partial
	from eliot._action import TaskLevel
	
	def wrapper(func):
		@wraps(func)
		def inner_func(self, *args, **kwargs):
			task_id = self.headers['X-Vaas-Eliot-Task']
			myself = _g_cluster.myself.netloc
			if task_id is None:
				with eliot.start_task(action_type=action_type if action_type is not None else 'eliot:resume_task', myself=myself, found_header=False):
					return func(self, *args, **kwargs)
			
			if isinstance(task_id, bytes):
				task_id = task_id.decode('ascii')
			uuid, task_level = task_id.split('@')
			action = eliot.Action(None, uuid, TaskLevel.fromString(task_level), action_type if action_type is not None else 'eliot:resume_task')
			fields['found_header'] = lambda: True
			action._start({ k: v() for k, v in fields.items() })
			with action:
				return func(self, *args, **kwargs)
	
		return inner_func
	return wrapper


class HTTPRequestHandler(SimpleHTTPRequestHandler):
	def do_GET(self):
		if self.path == '/':
			self.directory = 'static'
			super().do_GET()
		elif self.path == '/favicon.ico':
			self.directory = 'static'
			super().do_GET()
		elif self.path.startswith('/static/'):
			super().do_GET()
		elif self.path == '/cluster/host/':
			self.send('application/json', _g_cluster.dumps().encode('utf-8'))
		elif self.path == '/host/sync/':
			self.send('application/json', _g_cluster.myself.dumps().encode('utf-8'))
		elif self.path == '/channel':
			self.do_GET_channel()
		else:
			print('GET', self.path)
			raise NotImplementedError
	
	def do_GET_channel(self):
		# for debugging
		_, channel = self.path.split('/')
		assert _ == ''
		assert channel == 'channel'

		channel = _g_channels.make_local()

		self.send_response(200)
		self.send_header('Content-Type', 'text/plain')
		self.end_headers()

		content = channel.id.encode('utf-8')
		self.wfile.write(content + b'\n')
		self.wfile.flush()

		while True:
			content = channel.get()
			self.wfile.write(content + b'\n')
			self.wfile.flush()
	
	@continue_task_from_header('continue task from header', myself=lambda: _g_cluster.myself)
	def do_POST(self):
		length = self.headers['content-length']
		nbytes = int(length)
		data = self.rfile.read(nbytes)
		# throw away extra data? see Lib/http/server.py:1203-1205
		encoding = self.headers['content-encoding']
		if encoding is not None:
			if encoding == 'gzip':
				data = gzip.decompress(data)
		self.data = data
		
		if self.path == '/cluster/host/':
			self.do_POST_cluster_host()
		elif self.path.startswith('/service/job/'):
			self.do_POST_service_job()
		elif self.path.startswith('/channel/'):
			self.do_POST_channel()
		else:
			print('POST', self.path)
			raise NotImplementedError
		
	@eliot.log_call(action_type='do_POST_cluster_host')
	def do_POST_cluster_host(self):
		host = Host.loads(self.data)
		modified = _g_cluster.add(host)
		if modified:
			_g_cluster.broadcast_host(host)
			host.broadcast_host(_g_cluster.myself)
		
		content = 'ok\r\n'.encode('utf-8')
		
		self.send('text/plain', content)
	
	def do_POST_service_job(self):
		_, service, job, name = self.path.split('/', 3)
		assert _ == ''
		assert service == 'service'
		assert job == 'job'
		channel = self.headers['x-vaas-channel']

		with eliot.start_action(action_type='process incoming job', service_name=name, channel=channel) as action:
			self._do_POST_service_job(name, channel, action=action)
	
	def _do_POST_service_job(self, name, channel, *, action):
		action.log('received POST body', nbytes=len(self.data))

		service = Service.get_reference(name)

		if channel is not None:
			channel = RemoteChannel.loads(channel)
			if not channel.heartbeat():
				action.log('channel heartbeat failed')
				content = b'dead\r\n'
				self.send('text/plain', content, status=404)
				return
		else:
			channel = _g_channels.make_local()

		job = service.job_class.loads(self.data)
		args = (channel, _g_cluster)

		try:
			if channel.is_remote:
				action.log('this is being called by another instance')
				job(*args)
				content = b'ok\r\n'
				self.send('text/plain', content)
			
			else:
				# this introduces an action called 
				# "eliot:remote_task" which would be better if
				# it were called "eliot:preserve_context" as
				# that's more accurate.
				thread = Thread(target=eliot.preserve_context(job), args=(channel, _g_cluster))
				thread.start()
				
				self.send_response(200)
				self.send_header('Access-Control-Allow-Origin', self.headers['origin'])
				self.send_header('Content-Type', 'application/x-ndjson')
				self.send_header('X-VCI-Load', _g_cluster.dumps())
				if self._should_do_gzip():
					self.send_header('Content-Encoding', 'gzip')
				self.end_headers()
				
				wfile = self.wfile
				if self._should_do_gzip():
					wfile = gzip.open(wfile, 'wb')
				while thread.is_alive():
					try:
						content = channel.get(timeout=0.05)
					except Empty:
						continue

					action.log('sending partial result', nbytes=len(content))
					
					wfile.write(content + b'\n')
					wfile.flush()
		except BrokenPipeError:
			channel.close()
	
	def do_POST_channel(self):
		_, channel, id = self.path.split('/')
		assert _ == ''
		assert channel == 'channel'
		#assert id in _g_channels

		with eliot.start_action(action_type='process incoming channel write', channel_id=id) as action:
			self._do_POST_channel(id, action=action)
	
	def _do_POST_channel(self, id, *, action):
		try:
			channel = _g_channels[id]
		except KeyError:
			action.log('channel was closed')

			content = b'dead\r\n'
			self.send('text/plain', content, status=404)
		else:
			if self.data != b'heartbeat\r\n':
				action.log('wrote to channel', data_length=len(self.data))
				channel.put(self.data)
			else:
				action.log('heartbeat; channel still open')

			content = b'ok\r\n'
			self.send('text/plain', content)
	
	def send(self, content_type: str, content: bytes, *, status=200):
		self.send_response(status)
		if self._should_do_gzip():
			self.send_header('Content-Encoding', 'gzip')
			content = gzip.compress(content)
		self.send_header('Content-Type', content_type)
		self.send_header('Content-Length', str(len(content)))
		self.send_header('Access-Control-Allow-Origin', self.headers['origin'])
		self.end_headers()
		self.wfile.write(content)
	
	def _should_do_gzip(self):
		accept_encoding = self.headers.get('accept-encoding', None)
		if accept_encoding is not None:
			if 'gzip' in accept_encoding:
				return True
		return False


def main(docker, bind, host, port, service_name):
	global _g_port
	_g_port = port

	if bind is None:
		bind = host
	if bind == '0.0.0.0':
		myself = Host.from_system()
	else:
		myself = Host(f'{host}:{port}')
	
	cluster = Cluster.from_myself(myself)
	if docker:
		cluster.run_updater_thread(service_name)
	
	myself.run_updater_thread()
	
	channels = Channels.new()
	
	eliot.to_file(open(f'/logs/{myself.netloc}.log', 'wb'), encoder=JSONEncoder)
	
	global _g_cluster
	_g_cluster = cluster

	global _g_channels
	_g_channels = channels
	
	print(f'Listening on {bind}:{port}')
	address = (bind, port)
	server = ThreadingHTTPServer(address, HTTPRequestHandler)
	server.serve_forever()


def cli():
	import argparse
	
	parser = argparse.ArgumentParser()
	parser.add_argument('--docker', action='store_true')
	parser.add_argument('--service-name', default='vaas')
	parser.add_argument('--bind')
	parser.add_argument('--host', default='0.0.0.0')
	parser.add_argument('--port', type=int, default=8840)
	args = vars(parser.parse_args())
	
	main(**args)


if __name__ == '__main__':
	cli()
