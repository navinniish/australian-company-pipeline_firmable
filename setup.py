"""
Setup script for the Australian Company Pipeline package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="australian-company-pipeline",
    version="1.0.0",
    author="Australian Company Pipeline Team",
    author_email="noreply@example.com",
    description="ETL pipeline for Australian company data integration with LLM-powered entity matching",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/navinnniish/australian-company-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
            "pre-commit>=3.3.0",
        ],
        "monitoring": [
            "prometheus-client>=0.17.0",
            "grafana-api>=1.0.3",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "aus-company-pipeline=src.pipeline.etl_pipeline:main",
            "aus-company-extract=src.extractors.common_crawl_extractor:main",
            "aus-abr-extract=src.extractors.abr_extractor:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="etl, data-pipeline, australian-business-register, common-crawl, entity-matching, llm, machine-learning",
    project_urls={
        "Bug Reports": "https://github.com/navinniish/australian-company-pipeline/issues",
        "Source": "https://github.com/navinniish/australian-company-pipeline",
        "Documentation": "https://github.com/navinniish/australian-company-pipeline/blob/main/README.md",
    },
)