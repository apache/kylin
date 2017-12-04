#!/bin/bash

docker.build:
	docker build -f Dockerfile -t kylin-document:latest .

document.build:
	docker run --volume="$(shell pwd):/srv/jekyll" --rm -it kylin-document:latest jekyll build

runserver:
	docker run --volume="$(shell pwd):/srv/jekyll" -p 4000:4000 --rm -it kylin-document:latest jekyll server --watch

clean:
	rm -rf _site
