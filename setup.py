import os

from setuptools import setup, find_packages

setup_py_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(setup_py_dir)

setup(
    name='pytableau',
    version='1.0.0',
    packages=find_packages(),
    author='Memiiso',
    description='Python Tableau Wrapper',
    url='https://github.com/memiiso/pytableau',
    download_url='https://github.com/memiiso/pytableau/archive/master.zip',
    include_package_data=True,
    test_suite='tests',
    install_requires=['tableaudocumentapi', 'tableauserverclient>=0.9', 'PyPDF3', 'Pillow>=7.0.0'],
    python_requires='>=3',
)
