/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests;

import org.slf4j.Logger;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Wrapping class for {@link Process} that keeps textual in/out/err process streams.
 */
public class CliProcess
{
    private static final Logger LOGGER = getLogger(CliProcess.class);

    public static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final Scanner output;
    private final PrintStream input;
    private final Process process;

    public static List<String> trimLines(List<String> lines)
    {
        return lines.stream().map(String::trim).collect(toList());
    }

    public CliProcess(Process process)
    {
        this.output = new Scanner(process.getInputStream());
        this.input = new PrintStream(process.getOutputStream(), true);
        this.process = process;
    }

    public List<String> readRemainingLines()
    {
        List<String> lines = newArrayList();
        while (output.hasNextLine()) {
            lines.add(output.nextLine());
        }
        return lines;
    }

    public String nextOutputLine()
    {
        String nextLine = output.nextLine();
        LOGGER.debug("next line: {}", nextLine);
        return nextLine;
    }

    public String nextOutputToken()
    {
        String next = output.next();
        LOGGER.debug("next token: {}", next);
        return next;
    }

    public boolean outputHasNext(Pattern pattern)
    {
        return output.hasNext(pattern);
    }

    public PrintStream getInput()
    {
        return input;
    }

    /**
     * Waits for a process to finish and ensures it returns with 0 status. If the process
     * fails to finish within given timeout it is killed and {@link RuntimeException} is thrown.
     */
    public void waitWithTimeoutAndKill()
            throws InterruptedException
    {
        waitWithTimeoutAndKill(TIMEOUT);
    }

    public void waitWithTimeoutAndKill(Duration timeout)
            throws InterruptedException
    {
        if (!process.waitFor(timeout.toMillis(), MILLISECONDS)) {
            process.destroy();
            throw new RuntimeException("Child process didn't finish within given timeout");
        }

        int exitValue = process.exitValue();
        if (exitValue != 0) {
            throw new RuntimeException("Child process exited with non-zero code: " + exitValue);
        }
    }
}
