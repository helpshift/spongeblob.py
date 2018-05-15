# created in the ./.env directory and will be setup with all deps for
# running the tests.
clean:
	find . -name '*.pyc' -delete

test:
	tox -r

test_cloud:
	tox -r -- --no-docker
