import os
from setuptools import setup

directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(directory, 'README.md'), encoding='utf-8') as f:
  long_description = f.read()


setup(
    name='dataframe-service',
    packages=['dfs'],
    version='0.1.12',
    description='Pandas DataFrame Service (DFS).',
    author='Brian Guarraci',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires=['pandas~=1.5.1', 'pysimdjson~=5.0.2', 'colorama'],
    python_requires='>=3.8',
    include_package_data=True,
    test_suite='tests',
    scripts=['scripts/dfs_server', 'scripts/dfs_cli']
)
