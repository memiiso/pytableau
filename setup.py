import os

from setuptools import setup, find_packages

setup_py_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(setup_py_dir)

setup(
    name='pytableau',
    version='1.0.1',
    packages=find_packages(),
    author='Memiiso',
    description='Python Tableau Wrapper',
    url='https://github.com/memiiso/pytableau',
    download_url='https://github.com/memiiso/pytableau/archive/master.zip',
    include_package_data=True,
    test_suite='tests',
    install_requires=['pandas>=1.0.1', 'tableaudocumentapi>=0.7', 'tableauserverclient>=0.16.0', 'PyPDF3==1.0.1',
                      'Pillow==8.2.0', "openpyxl==3.0.4"],
    python_requires='>=3.7',
)
