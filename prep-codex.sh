pip install -r requirements.txt
maturin build --profile=fast -i python3
pip install target/wheels/*.whl
