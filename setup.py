from setuptools import setup, find_packages
setup(
    name="socialcasting",
    version="0.1",
    packages=find_packages(),
    install_requires=['pandas', 'requests'],
    setup_requires=['pytest-runner'],
    tests_requires=['pytest', 'responses']
)
