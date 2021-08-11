## Proposed changes

Describe the big picture of your changes here to communicate to the maintainers why we should accept this pull request. If it fixes a bug or resolves a feature request, be sure to link to that issue.

## Github Branch 

As most of the development works are on Kylin 4, we need to switch it as main branch. Apache Kylin community changes the branch settings on Github since 2021-08-04 :

1. The original branch _kylin-on-parquet-v2_ for **Kylin 4.X** (Parquet Storage) has been renamed to branch **main**, and configured as the **default** branch;
2. The original branch _master_ for **Kylin 3.X** (HBase Storage) has been renamed to branch **kylin3** ;

Please check [Intro to Kylin 4 architecture](https://kylin.apache.org/blog/2021/07/02/Apache-Kylin4-A-new-storage-and-compute-architecture/) and [INFRA-22166](https://issues.apache.org/jira/browse/INFRA-22166) if you are interested.

## Types of changes

What types of changes does your code introduce to Kylin?
_Put an `x` in the boxes that apply_

- [ ] Bugfix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation Update (if none of the other choices apply)

## Checklist

_Put an `x` in the boxes that apply. You can also fill these out after creating the PR. If you're unsure about any of them, don't hesitate to ask. We're here to help! This is simply a reminder of what we are going to look for before merging your code._

- [ ] I have create an issue on [Kylin's jira](https://issues.apache.org/jira/browse/KYLIN), and have described the bug/feature there in detail
- [ ] Commit messages in my PR start with the related jira ID, like "KYLIN-0000 Make Kylin project open-source"
- [ ] Compiling and unit tests pass locally with my changes
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] I have added necessary documentation (if appropriate)
- [ ] Any dependent changes have been merged

## Further comments

If this is a relatively large or complex change, kick off the discussion at user@kylin or dev@kylin by explaining why you chose the solution you did and what alternatives you considered, etc...
