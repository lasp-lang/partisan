FROM erlang:20.3

MAINTAINER Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>

# Build prereq's.
RUN cd /tmp && \
    apt-get update && \
    apt-get -y install wget build-essential make gcc ruby-dev git expect gnuplot tmux strace && \
    gem install gist

# Build.
RUN cd /opt && \
    (git clone https://github.com/lasp-lang/partisan.git -b rename-backend && cd partisan && make rel);

# Run.
CMD cd /opt/partisan && \
    chmod 755 /opt/partisan/_build/default/rel/partisan/bin/env && \
    /opt/partisan/_build/default/rel/partisan/bin/env