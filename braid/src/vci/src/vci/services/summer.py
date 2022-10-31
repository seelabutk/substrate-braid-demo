"""

"""

from .. import Service, Job, Result, Host
import json

__all__ = ['summer']


summer: Service = Service('summer')


@summer.register
@dataclass(frozen=True)
class SummerResult(Result):
	sum: int
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		sum = data['sum']
		return cls(sum)
	
	def dumps(self):
		data = {}
		data['sum'] = self.sum
		return json.dumps(data)


@summer.register
@dataclass(frozen=True)
class SummerJob(Job):
	limit: int
	count: int
	
	@classmethod
	def loads(cls, data):
		data = json.loads(data)
		limit = data['limit']
		count = data['count']
		return cls(limit, count)
	
	def dumps(self):
		data = {}
		data['limit'] = self.limit
		data['count'] = self.count
		return json.dumps(data)
	
	def __call__(self, cluster: Cluster) -> SummerResult:
		sleep(1)
		print(self)
		host = cluster.hosts[self.count % len(cluster.hosts)]
		if self.count < self.limit:
			job = SummerJob(self.limit, self.count + 1)
			result = host.submit(summer, job)
			return SummerResult(result.sum + self.count)
		else:
			return SummerResult(self.count)
