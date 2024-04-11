from setuptools import setup

setup(
    name='bi_publishing',
    version='0.1.0',
    description='Utilities for publishing PowerBI workspaces, reports and datasets.',
    url='https://github.com/cienai/cien-bi-publishing',
    author='cien',
    author_email='dev@cien.ai',
    packages=[
        'bi_publishing'
    ],
    install_requires=[
        "msal"],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
