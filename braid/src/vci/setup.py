from distutils.core import setup

setup(
	name='vci',
	version='0.1.0',
	packages=[
		'vci',
		'vci.services',
	],
	package_dir={
		'vci': 'src/vci',
		'vci.services': 'src/vci/services',
	},
	requires=[
		'requests',
		'dnspython',
		'eliot',
		'psutil',
	],
)
