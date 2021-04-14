import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyzzle-just-analytics",
    version="0.1.0",
    author="Example Author",
    author_email="van.vu@justanalytics.com",
    description="Python ETL support for Databricks Delta Lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/blackslender/pyzzle",
    project_urls={
        "Bug Tracker": "https://github.com/blackslender/pyzzle/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Databricks Clusters",
    ],
    package_dir={"": "source"},
    packages=setuptools.find_packages(where="source"),
    python_requires=">=3.6",
)
