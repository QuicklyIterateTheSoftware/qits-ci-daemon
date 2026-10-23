package eu.wohlben.qits.cidaemon;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The exclusive lock on one mirror, held on {@code /cache/git/<projectId>/<repoName>.lock} — the
 * dossier's {@code flock}, taken in-process with {@link FileChannel#lock()} rather than by spawning
 * {@code flock(1)}: the mirror's clone-or-fetch, its {@code worktree prune} and the {@code worktree
 * add} are several git invocations that must all sit under one lock, and a child process holding a
 * lock for its own lifetime cannot span them without assembling a shell line around them.
 *
 * <p>Two daemons on one target share the cache volume, so the lock must hold across processes, and
 * that is the file lock. It must hold <em>inside</em> one process too — the suite runs two
 * preparations in one JVM, and the JDK refuses a second {@code FileLock} on a file the same JVM
 * already locks ({@code OverlappingFileLockException}) instead of blocking — so a {@link
 * ReentrantLock} per path is taken first.
 *
 * <p>The file is a POSIX record lock ({@code fcntl}), which on Linux does not exclude a {@code
 * flock(1)} on the same path. Every user of these lock files is this class, so the two never meet;
 * a shell script that wanted to join in would have to use the same kind of lock.
 *
 * <p>The lock file is never deleted. Deleting it while another process waits on the old inode is
 * how two holders end up with one lock each.
 */
final class GitLock implements AutoCloseable {

  private static final ConcurrentHashMap<Path, ReentrantLock> IN_PROCESS =
      new ConcurrentHashMap<>();

  private final ReentrantLock local;
  private final FileChannel channel;
  private final FileLock lock;

  private GitLock(ReentrantLock local, FileChannel channel, FileLock lock) {
    this.local = local;
    this.channel = channel;
    this.lock = lock;
  }

  /** Block until {@code file} is ours. Creates it, and its directory, when absent. */
  static GitLock acquire(Path file) throws IOException {
    Path key = file.toAbsolutePath().normalize();
    ReentrantLock local = IN_PROCESS.computeIfAbsent(key, k -> new ReentrantLock());
    local.lock();
    FileChannel channel = null;
    try {
      Files.createDirectories(key.getParent());
      channel = FileChannel.open(key, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      return new GitLock(local, channel, channel.lock());
    } catch (IOException | RuntimeException e) {
      if (channel != null) {
        try {
          channel.close();
        } catch (IOException ignored) {
          // the failure below is the one worth reporting
        }
      }
      local.unlock();
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      lock.release();
      channel.close();
    } catch (IOException ignored) {
      // Closing the channel releases the lock whatever release() said.
    } finally {
      local.unlock();
    }
  }
}
