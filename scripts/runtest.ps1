Remove-Item -Recurse -Force *\__pycache__
Remove-Item -Recurse -Force .pytest_cache
poetry update
poetry install
poetry run pytest --cov=quez --cov-report=term-missing

