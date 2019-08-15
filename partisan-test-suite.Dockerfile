FROM cmeiklejohn/partisan-base:latest

MAINTAINER Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>

# Build from GitHub.
RUN cd /opt && \
    (git clone https://github.com/lasp-lang/partisan.git -b rename-backend && cd partisan && make rel);

# Run.
CMD cd /opt/partisan && \
    make test