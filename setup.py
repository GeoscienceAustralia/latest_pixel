"""
Setup
-----

"""
import os

from setuptools import setup, find_packages

setup(name='agdc_pixel',
      version=os.environ.get('version', 0.0),
      description='Geoscience Australia - pixel-rollover for AGDC',
      license='Apache License 2.0',
      url='https://github.com/GeoscienceAustralia/latest_pixel',
      author='AGDC Collaboration',
      maintainer='AGDC Collaboration',
      maintainer_email='',
      packages=find_packages(),
      install_requires=[
          'datacube',
      ],
      entry_points={
          'console_scripts': [
              'datacube-pixel = agdc_pixel.pixel_app:main',
          ]
      })
