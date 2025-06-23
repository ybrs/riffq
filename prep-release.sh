#!/bin/bash
# for example release-0.1.3-dev20250623+1
export GITHUB_REF_NAME=$1
python -c "import os,re,pathlib;v=os.environ['GITHUB_REF_NAME'][len('release-'):];p=pathlib.Path('Cargo.toml');p.write_text(re.sub(r'^version\\s*=.*$',f'version = \"{v}\"',p.read_text(),flags=re.M))"
git add Cargo.toml
git commit -m "release: ${GITHUB_REF_NAME}"
git tag $GITHUB_REF_NAME
git push origin main --tags