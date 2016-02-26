/**
 * CPE 369, Lab 1 Part 1, 1/10/2016
 * Waylin Wang, Myron Zhao
 */

import javax.json.*;
import javax.json.stream.JsonGenerator;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;

public class thghtShreGen {

    public static void main(String[] args) {
        String outputFile;
        String inputFile;
        int numJSONObjectToGenerate;
        FileWriter fw;
        BufferedReader reader;
        String line;
        ArrayList<String> lines = new ArrayList<>();

        //error checking
        if (args.length < 2) {
            System.out.println("Please check README for usage");
            return;
        }

        //Take input parameters from command line
        outputFile = args[0];
        numJSONObjectToGenerate = Integer.parseInt(args[1]);

        //Choose file to read from
        if (args.length > 2) {
            inputFile = args[2];
        }
        else {
            inputFile = "sense.txt";
        }

        //Make sure command line arguments are valid
        if (outputFile.isEmpty() || numJSONObjectToGenerate == 0) {
            System.out.println("Please check README for usage");
            return;
        }

        //Read lines from input file
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        catch (Exception e){
            System.out.println("Error reading from " + inputFile + ". Please try again!");
            e.printStackTrace();
            return;
        }

        //Setup file output writer
        JsonWriter writer;
        PrintWriter pWriter;
        try {
            fw = new FileWriter(outputFile);
        }
        catch (Exception e) {
            System.out.println("Create file failed. Please try again!");
            return;
        }

        //Configures settings to pretty-print JSON objects
        Map<String, Boolean> config = new HashMap<>();
        config.put(JsonGenerator.PRETTY_PRINTING, true);

        JsonWriterFactory factory = Json.createWriterFactory(config);

        //Create JsonWriter
        try {
            writer = factory.createWriter(fw);
            pWriter = new PrintWriter(fw);
        }
        catch (Exception e) {
            System.out.println("Create file failed. Please try again!");
            return;
        }


        // Write punctuation
//        try {
//            pWriter.write("[");
//        } catch (Exception e) {
//            System.out.println("Write to file failed. Please try running program again!");
//            return;
//        }

        //Generate JSON objects
        JsonObject obj;
        Random rng = new Random();
        Message msg = new Message();
        int curMsg = 1;

        for (int i = 0; i < numJSONObjectToGenerate; i++) {
            curMsg += rng.nextInt(5) + 1;
            msg.genMessage(curMsg, rng, lines);

            //Determine if message is in-response and create JSON object accordingly
            if (msg.getInResponse() != -1) {
                obj = Json.createObjectBuilder()
                        .add("messageID", msg.getMessageId())
                        .add("user", msg.getUser())
                        .add("status", msg.getStatus())
                        .add("recepient", msg.getRecepient())
                        .add("in-response", msg.getInResponse())
                        .add("text", msg.getText())
                        .build();
            }
            else {
                obj = Json.createObjectBuilder()
                        .add("messageID", msg.getMessageId())
                        .add("user", msg.getUser())
                        .add("status", msg.getStatus())
                        .add("recepient", msg.getRecepient())
                        .add("text", msg.getText())
                        .build();
            }

            // Write JSON object to file
            try {
                writer = factory.createWriter(fw);
                writer.write(obj);
            } catch (Exception e) {
                System.out.println("Write to file failed. Please try running program again!");
                e.printStackTrace();
                return;
            }


            // Write punctuation
//            if (i != numJSONObjectToGenerate - 1) {
//                try {
//                    pWriter.write(",");
//                } catch (Exception e) {
//                    System.out.println("Write to file failed. Please try running program again!");
//                    return;
//                }
//            }

        }


        // Write punctuation
//        try {
//            pWriter.write("]");
//        } catch (Exception e) {
//            System.out.println("Write to file failed. Please try running program again!");
//            return;
//        }

        pWriter.close();
        writer.close();
    }
}

class Message {

    private int messageId, inResponse;
    private String user, status, recepient, text;

    public Message() {
    }

    public Message(int messageId, int inResponse, String user, String status, String recepient, String text) {
        this.messageId = messageId;
        this.inResponse = inResponse;
        this.user = user;
        this.status = status;
        this.recepient = recepient;
        this.text = text;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public int getInResponse() {
        return inResponse;
    }

    public void setInResponse(int inResponse) {
        this.inResponse = inResponse;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRecepient() {
        return recepient;
    }

    public String setRecepient(String recepient) {
        this.recepient = recepient;
        return this.recepient;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void genMessage (int curMsg, Random rng, ArrayList<String> lines) {
        int rngNum;
        int numWords;
        String line = "";
        this.setMessageId(curMsg);
        this.setUser("u" + (rng.nextInt(10000) + 1));

        //Randomly set a status type
        if ((rngNum = rng.nextInt(100)) < 80) {
            this.setStatus("public");
        }
        else if (rngNum < 90) {
            this.setStatus("private");
        }
        else {
            this.setStatus("protected");
        }

        //Randomly generate recepient for public statuses
        if (this.getStatus().equals("public")) {
            if ((rngNum = rng.nextInt(100)) < 40) {
                this.setRecepient("all");
            }
            else if (rngNum < 80) {
                this.setRecepient("subscribers");
            }
            else if (rngNum < 95) {
                this.setRecepient("u" + (rng.nextInt(10000) + 1));
            }
            else {
                this.setRecepient("self");
            }
        }

        //Randomly generate recepient for private statuses
        if (this.getStatus().equals("private")) {
            if ((rngNum = rng.nextInt(100)) < 90) {
                while (this.setRecepient("u" + (rng.nextInt(10000) + 1)).equals(this.getUser())){
                    ;
                }
            }
            else {
                this.setRecepient("self");
            }
        }

        //Randomly generate recepient for protected statuses
        if (this.getStatus().equals("protected")) {
            if ((rngNum = rng.nextInt(100)) < 85) {
                this.setRecepient("subscribers");
            }
            else if (rngNum < 95) {
                while (this.setRecepient("u" + (rng.nextInt(10000) + 1)).equals(this.getUser())){
                    ;
                }
            }
            else {
                this.setRecepient("self");
            }
        }

        //Randomly decide whether or not a message is in-response
        if (rng.nextInt(100) < 70) {
            this.setInResponse(rng.nextInt(curMsg) - 1);
        }
        else {
            this.setInResponse(-1);
        }

        //Randomly generate string of text from input file
        numWords = rng.nextInt(19) + 2;

        while (numWords-- > 0) {
            line += lines.get(rng.nextInt(lines.size()));
            if (numWords != 0) {
                line += " ";
            }
        }

        this.setText(line);
    }
}