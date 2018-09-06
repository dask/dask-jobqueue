Dask-jobqueue heavily relies on dask and distributed upstream projects.  
We may want to check their status while releasing.


Release for dask-jobqueue, from within your fork:

* Update release notes in `docs/source/changelog.rst`. Preferably within a PR,
which informs of incoming release.

* Once PR is merge, checkout master branch:

````
git checkout upstream/master
````

* Create a tag and push to github:

````
git tag -a x.x.x -m 'Version x.x.x'
git push --tags upstream
````

* Build the wheel/dist and upload to PyPI:

````
git clean -xfd
python setup.py sdist bdist_wheel --universal
wine upload dist/*
````

* Conda forge bots should pick up the change automatically. Just follow
instructions from automatic email that you should receive:
  * check that dependencies have not changed,
  * Merge the PR on conda-forge/dask-jobqueue-feedstock once tests have passed.

