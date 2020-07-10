import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="filoc",
    version="0.0.8",
    author="jeromerg",
    author_email="jeromerg@gmx.net",
    description="eases the saving and reading of files within a structured folder tree",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jeromerg/filoc",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'fsspec',
        'parse',
        'frozendict',
        'orderedset',
    ],
    python_requires='>=3',
)