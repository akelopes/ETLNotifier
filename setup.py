from setuptools import setup, find_packages

setup(
    name="etl_notifier",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyodbc",
        "pyyaml",
        "requests",
        "python-dotenv",
        "tenacity",
        "pydantic",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pytest-asyncio",
            "black",
            "isort",
            "mypy",
            "flake8",
            "requests-mock",
            "aiohttp",
            "types-requests",
            "types-PyYAML",
        ]
    },
    python_requires=">=3.8",
)