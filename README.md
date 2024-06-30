Steps for local setup:
```sh
pyenv install 3.12.2
pyenv virtualenv 3.12.2 maestria

pyenv activate maestria
pip install --upgrade pip setuptools tox
```

run: 
```sh
tox
```
