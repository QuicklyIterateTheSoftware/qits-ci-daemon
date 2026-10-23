package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonBinary;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * {@code --version} is what the release runs on the built binary in place of the old container
 * smoke test, so it must answer before Quarkus starts — with no environment, no config and no
 * socket — and print the name and the version and nothing else.
 */
class MainTest {

  @Test
  void versionPrintsTheBinarysNameAndVersionWithoutStartingTheDaemon() {
    PrintStream previous = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      Main.main("--version");
    } finally {
      System.setOut(previous);
    }

    assertEquals(
        "qits-ci-daemon " + CiDaemonBinary.VERSION + System.lineSeparator(),
        captured.toString(StandardCharsets.UTF_8));
  }
}
