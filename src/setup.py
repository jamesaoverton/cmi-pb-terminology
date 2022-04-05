from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
install_requires = [
    "Flask==2.0.2",
    "lark==1.0.0",
    "SQLAlchemy==1.4.29",
    "xlsx2csv",
    # "ontodev-gizmos @ git+https://github.com/ontodev/gizmos.git",
    # "ontodev-sprocket @ git+https://github.com/ontodev/sprocket.git"
]

setup(
    name="cmi-pb-terminology",
    version="0.0.1",
    description="Terminology tools for the Computational Modelling of Immunology Pertussis Boost project.",
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
    package_data={"cmi_pb_server": ["templates/*.html"]},
)
