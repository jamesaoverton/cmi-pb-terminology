from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

install_requires = ["ontodev-gizmos"]

setup(
    name="cmi-pb-terminology",
    version="0.0.1",
    description="Terminology tools for the Computational Modelling of Immunology Pertussis Boost project.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jamesaoverton/cmi-pb-terminology",
    author="James A. Overton",
    author_email="james@overton.ca",
    classifiers=[  # Optional
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=install_requires,
    packages=find_packages(exclude="test"),
    python_requires=">=3.6, <4",
)
