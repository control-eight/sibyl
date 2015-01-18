package com.my.sibyl.itemsets.kiji.tests;

import org.kiji.schema.util.ResourceUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * A source of input that delivers prompts to the user and reads responses from the console.
 * @author abykovsky
 * @since 12/20/14
 */
public final class ConsolePrompt implements Closeable {

    /** The underlying reader instance. */
    private BufferedReader mConsoleReader;

    /** Initialize a ConsolePrompt instance. */
    public ConsolePrompt() {
        try {
            mConsoleReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        } catch (UnsupportedEncodingException uee) {
            // UTF-8 is specified as supported everywhere on Java; should not get here.
            throw new IOError(uee);
        }
    }

    /**
     * Display a prompt and then wait for the user to respond with input.
     *
     * @param prompt the prompt string to deliver.
     * @return the user's response.
     * @throws IOError if there was an error reading from the console.
     */
    public String readLine(String prompt) {
        System.out.print(prompt);

        try {
            return mConsoleReader.readLine();
        } catch (IOException ioe) {
            throw new IOError(ioe);
        }
    }

    /**
     * Close all underlying resources.
     */
    @Override
    public void close() {
        ResourceUtils.closeOrLog(mConsoleReader);
    }
}
