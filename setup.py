from setuptools import setup

setup(name='midas',
      version='1.1.0',
      description='The MIDAS (Modular Integrated Distributed Analysis System) is a system for online analysis of streaming signals and allows easy integration of such into machine learning frameworks.',
      author='Andreas Henelius, Jari Torniainen, Brain Work Research Center at the Finnish Institute of Occupational Health',
      author_email='andreas.henelius@ttl.fi, jari.torniainen@ttl.fi',
      url='https://github.com/bwrc/midas',
      license='MIT',
      packages=['midas'],
      package_dir={'midas': 'midas'},
      include_package_data=False,
      install_requires = ['bottle>=0.12',
                          'PyZMQ>=14.3.1',
                          'Waitress>=0.8.9',
                          'pylsl>=1.10.4'],
      entry_points={"console_scripts":
                    ["midas-dispatcher = midas.dispatcher:run_from_cli"]}
)
