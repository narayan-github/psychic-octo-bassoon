# Lab-2: Healthcare Claims ETL Pipeline

A minimal placeholder/demo showcasing a GitHub Actions CI/CD pipeline for healthcare claims processing.

## Architecture

Hospital DB → Python ETL → Azure SQL

## Features

- Automated CI/CD pipeline with GitHub Actions
- Code quality checks with flake8
- Test execution with pytest
- Coverage reporting
- Environment variable management
- Virtual environment support

## Setup Instructions

### 1. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the `.env` file and update with your credentials:

```bash
cp .env .env.local
```

Edit `.env.local` with your actual database credentials:
```
DB_HOST=your_database_host
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### 4. Run the ETL Pipeline

```bash
python etl_pipeline.py
```

### 5. Run Tests

```bash
pytest --cov=.
```

### 6. Run Linting

```bash
flake8 .
```

## GitHub Actions Workflow

The CI pipeline is triggered on pull requests and includes:

- **Setup Python**: Configures Python 3.11 environment
- **Install Dependencies**: Installs packages from requirements.txt
- **Run Lint**: Executes flake8 for code quality checks
- **Run Tests**: Executes pytest with coverage reporting

## Project Structure

```
.
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions workflow
├── tests/
│   ├── __init__.py
│   └── test_etl_pipeline.py  # Pytest test cases
├── etl_pipeline.py         # ETL pipeline code
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables template
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## ETL Pipeline Components

- **Extract**: Retrieves healthcare claims from source database
- **Transform**: Validates and transforms claims data
- **Load**: Loads processed claims into target database

## Learning Outcomes

- AI-generated YAML for GitHub Actions
- GitHub Actions automation
- CI/CD pipeline configuration
- Test-driven development with pytest
- Code quality enforcement with flake8
