import setuptools

setuptools.setup(
    name="sport-betting",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
