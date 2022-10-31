#!/usr/bin/env python3.7
"""

"""

from __future__ import annotations
from collections import Counter, defaultdict


class NotEnoughHosts(Exception):
	pass


def assign_with_redundancy(nhosts: int, nparts: int, redundancy: int) -> Dict[HostId, List[PartId]]:
	assigns: Dict[HostId, List[PartId]] = defaultdict(list)

	def has_redundancy():
		counts: Dict[PartId, int] = Counter()
		for i in range(nhosts):
			counts.update(assigns[i])

		for i in range(nparts):
			if counts[i] < redundancy:
				break
		else:
			return True

		return False

	def has_fair_load(within=1):
		counts: Dict[HostId, int] = Counter()
		for i in range(nhosts):
			counts[i] += len(assigns[i])

		return max(counts.values()) - min(counts.values()) <= within
	
	def which_has_least_redundancy() -> PartId:
		counts: Dict[PartId, int] = Counter()
		for i in range(nhosts):
			counts.update(assigns[i])
		
		least_part: PartId = 0
		least_count: int = counts[least_part]
		for part in range(1, nparts):
			if counts[part] < least_count:
				least_part = part
				count = counts[least_part]

		return least_part

	def which_has_least_load_and_not_that_part(part: PartId) -> HostId:
		hosts_sans_that_part: List[HostId] = []
		for i in range(nhosts):
			if part in assigns[i]:
				print(f'<Host {i}> has <Part {part}>: {assigns[i]}')
				continue
			hosts_sans_that_part.append(i)

		if len(hosts_sans_that_part) == 0:
			raise NotEnoughHosts(f'There were not enough hosts to assign partitions to; '
			                     f'consider adding more hosts or decreasing the amount of redundancy')
			
		counts: Dict[HostId, int] = Counter()
		for host in hosts_sans_that_part:
			counts[host] += len(assigns[host])

		least_host: HostId = hosts_sans_that_part[0]
		least_count: int = counts[least_host]
		for host in hosts_sans_that_part:
			if counts[host] < least_count:
				least_host = host
				count = counts[least_host]

		return least_host
	
	def has_no_duplicates():
		for host in range(nhosts):
			if len(assigns[host]) == 0:
				print(f'Perhaps a problem? <Host {host}> does not have any parts assigned')
				print(repr(assigns))
			counts = Counter(assigns[host])
			part, count = counts.most_common(1)[0]
			if count > 1:
				print(f'<Host {host}> has a duplicate: <Part {part}>')
				break
		else:
			return True

		return False

	if nparts == 0:
		print('partition.py: Warning: nparts is zero')
	
	if nhosts == 0:
		print('partition.py: Warning: nhosts is zero')

	while True:
		if has_fair_load() and has_redundancy():
			break

		part = which_has_least_redundancy()
		host = which_has_least_load_and_not_that_part(part)

		assigns[host].append(part)
	
	assert has_no_duplicates()
	return assigns
