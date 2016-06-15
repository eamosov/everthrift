package org.everthrift.sql.migration.utils;

import java.io.IOException;


    public class ConsoleUtils
    {

        private static final String CONSOLE = ": ";
        public static final void printString(String format,Object... args){
            System.out.printf(format,args);
        }

        public static final boolean readYN(String prompt) {
            String input = readChar(prompt).toLowerCase().trim();
            if (input.equals("y") || input.equals("Y")) {
                return true;
            }
            return false;
        }


        private static final String readLine(String prompt) {
            System.out.print(prompt);
            System.out.print(CONSOLE);

            StringBuffer b = new StringBuffer();
            while(true) {
                try {
                    char c = (char) System.in.read();
                    b.append(c);
                    if (c == '\n') {
                        return b.toString().trim();
                    } else if (c == '\r') { }
                } catch (IOException e) { }
            }
        }

        private static final String readChar(String prompt) {
            String line = readLine(prompt);
            return line.length()>0?line.substring(0,1):line;
        }



}
