from setuptools import setup, find_packages

setup(
    name="edgetrailresearch",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "yfinance",
        "numpy",
        "matplotlib",
        "fredapi",
        "sqlalchemy",
        "datetime"
    ],
) 