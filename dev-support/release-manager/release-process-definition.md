## Definition of release process for 5.X


### All Steps
- [ ] Check all issue of this release version whether they have been resolved
- [ ] Code freeze and running CI to make sure it succeed.
- [ ] Release maven artifacts using maven-release-plugin
- [ ] Release source code to svn(candidate path)
- [ ] Release binary to svn(candidate path)
- [ ] Call a vote for current RC in dev mailing list
- [ ] Announce RC 's result of a vote
- [ ] Close all related issues 
- [ ] Upload release to svn(release path)
- [ ] Update website, including release notes and downloads link(checksum, signature etc)
- [ ] Announce release in mailing list(user/dev)
- [ ] (Optional for small release) Build and publish standalone docker image for this release