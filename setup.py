# !/usr/bin/env python
# python setup.py bdist_wheel
import setuptools
setuptools.setup(
    name='covid',
    packages=setuptools.find_packages(),
    version='0.0.0',
    description='Covid data exploration',
    author='Ryan D. Batt',
    license='MIT',
    author_email='battrd@gmail.com',
    url='https://github.com/rBatt/covid',
    keywords=['covid', 'coronavirus', 'package', ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
    ],
)