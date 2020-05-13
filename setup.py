from setuptools import setup,find_packages

def readme():
    with open('README.md','r') as fh:
        return fh.read()
        
setup(
    author = 'JaylanLiu',
    author_email = 'liujilong@genomics.cn',

    name = 'HuoYan_monitoring',
    version = '0.1',
    description='HuoYan laboratory COVID-19 samples testing lifetime monitoring',
    long_description=readme(),
    url='https://github.com/JaylanLiu/HuoYan_monitoring',

    packages = find_packages(),
    install_requires=['pandas','numpy','dash','dash-auth','apscheduler'],

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)