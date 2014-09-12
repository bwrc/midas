from setuptools import setup

setup(name='midas',
      version='0.1.1',
      description='The MIDAS (Modular Integrated Distributed Analysis System) is a system for online analysis of streaming signals and allows easy integration of such into machine learning frameworks.',
      author='Andreas Henelius, Jari Torniainen, Brain Work Research Center at the Finnish Institute of Occupational Health',
      author_email='andreas.henelius@ttl.fi, jari.torniainen@ttl.fi',
      url='http://github.io/FIOH-BWRC/midas',
      license='MIT',
      packages=['midas'],
      package_dir = {'midas': 'midas'},
      package_data={'midas' : ['midas/liblsl32.dll','midas/liblsl32.dylib','midas/liblsl64.dll', 'midas/liblsl64.dylib', 'midas/liblsl64.so']},
      include_package_data=True,


)



