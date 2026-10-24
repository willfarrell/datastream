# Never built. Dependabot can't update `container:` images in workflow files
# (dependabot-core#5819), so this FROM line is the single source of the semgrep
# job's image: Dependabot (docker ecosystem) bumps it and
# .github/workflows/test-sast.yml reads it.
FROM semgrep/semgrep:1.111.0@sha256:3e6e5065d9e68abffddffdb536a8db2d79a8fa92dc424daa48d3a4b7d9bc65d0
