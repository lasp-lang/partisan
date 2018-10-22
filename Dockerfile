FROM erlang:20.3

MAINTAINER Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>

# Build prereq's.
RUN cd /tmp && \
    apt-get update && \
    apt-get -y install wget build-essential make gcc ruby-dev git expect gnuplot tmux strace && \
    gem install gist

# Copy into the container the partisan code.
COPY . /opt/partisan

# Verify output.
CMD ls /opt