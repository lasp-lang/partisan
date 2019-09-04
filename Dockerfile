FROM cmeiklejohn/partisan-base:latest

MAINTAINER Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>

# Build from GitHub.
RUN cd /opt && \
    (git clone https://github.com/lasp-lang/partisan.git -b rename-backend && cd partisan && make rel);

# Run.
CMD cd /opt/partisan && \
    chmod 755 /opt/partisan/_build/test/rel/partisan/bin/env && \
    /opt/partisan/_build/test/rel/partisan/bin/env
