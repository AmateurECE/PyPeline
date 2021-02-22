import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypeline",
    version="0.2.0",
    author="Ethan Twardy",
    author_email="ethan.twardy@gmail.com",
    description="Framework for creating generic data pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AmateurECE/PyPeline",
    packages=['pypeline'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pyyaml',
        'cerberus',
    ],
    provides=['pypeline'],
    entry_points={
        'console_scripts': [
            'pypeline=pypeline.Pipeline:main',
        ]
    }
)
