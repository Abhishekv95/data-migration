from setuptools import setup, find_packages

setup(
    name="data_migration_utility",
    version="0.1.0",
    author="Your Name",
    description="A utility to migrate data from MySQL to PostgreSQL",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "pymysql",
        "psycopg2",
        "sqlalchemy",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
