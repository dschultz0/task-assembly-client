import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name="task-assembly",
    version='0.1.0',
    author="Dave Schultz",
    author_email="dave@daveschultzconsulting.com",
    description="SDK and CLI for using the Task Assembly crowdwork service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dschultz0/task-assembly-client",
    packages=setuptools.find_packages(exclude=['test']),
    include_package_data=False,
    keywords="task assembly, mturk",
    python_requires=">=3.8",
    install_requires=[
        "api-client",
        "larry",
        "boto3"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
