/**
 * CPE 369, Lab 1 Part 1, 1/10/2016
 * Waylin Wang, Myron Zhao
 */

import javax.json.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;


public class beFuddledGen {

    public static void main(String[] args) throws IOException {
        String outputFile; //Path length of the output file
        int numJSONObjectToGenerate; //Number of json objects to generate

        //If didn't provide enough params, just return
        if (args.length != 2) {
            System.out.println("Please check README for usage");
            return;
        }

        outputFile = args[0];
        numJSONObjectToGenerate = Integer.parseInt(args[1]);

        //If provided bad params, just return
        if (outputFile.isEmpty() || numJSONObjectToGenerate == 0) {
            System.out.println("Please check README for usage");
            return;
        }

        //Setup file output writer
        PrintWriter writer;

        try {
            writer = new PrintWriter(new FileWriter(outputFile));
        }
        catch (Exception e) {
            System.out.println("Create file failed. Please try again!");
            return;
        }

        //add Game objects to the list of games we need to generate
        ArrayList<User> users = generateGameList(numJSONObjectToGenerate);

        //prints the json array opening
//        try {
//            writer.println("[");
//        } catch (Exception e) {
//            System.out.println("Write to file failed. Please try running program again!");
//            return;
//        }


        //generate the json object and write to file
        int curGameNumber = 1;
        //while there are more json objects to generate and the users list is not empty yet
        while (numJSONObjectToGenerate-- > 0 && !users.isEmpty()) {
            User curUser;
            //find the next user by random that has a game queued for him
            //if the user found does not have anymore games, remove the user from the list
            do {
                curUser = users.get((int) (Math.random() * users.size()));
            } while (!curUser.hasGameInQue() && users.remove(curUser));

            //set the game number for the current game in accordance to Rule 2
            if (!curUser.isGameStarted()) {
                curUser.setGameNumber(curGameNumber++);
            }

            //Calls the step method that returns a String to be printed to file
            try {
                if (numJSONObjectToGenerate == 0) {
                    writer.println(curUser.step());
                }
                else {
                    writer.println(curUser.step());
                    //writer.println(curUser.step() + ",");
                }
            } catch (Exception e) {
                System.out.println("Write to file failed. Please try running program again!");
                return;
            }
            // clean up for the current user
            if (!curUser.hasGameInQue()) users.remove(curUser);

            if (users.isEmpty() && numJSONObjectToGenerate > 0){
                users = generateGameList(Integer.parseInt(args[1]));
            }
        }

        //prints json array end bracket
//        try {
//            writer.println("]");
//        } catch (Exception e) {
//            System.out.println("Write to file failed. Please try running program again!");
//            return;
//        }

        writer.close();
    }

    /**
     * This function takes in the number of JSON objects to generate
     * and returns an arraylist of users with qued games
     * @param numJSONObjectToGenerate
     * @return ArrayList of Users that has games qued up for them
     */
    private static ArrayList<User> generateGameList(int numJSONObjectToGenerate) {
        ArrayList<User> retVal = new ArrayList<>();
        Random gen = new Random();

        //Calculates the total number of games by generating a random number from a normal distribution
        //with mean of 36 and std dev of 5, then right shifted by 9 to guarantee that we have the minimum
        //number of steps required.
        int totalGames = numJSONObjectToGenerate / (Math.abs((int) (gen.nextGaussian() * 5 + 36)) + 9);

        //Loop to generate all the games
        for (int i = 1; i <= totalGames; i++) {
            //Max step is random from normal distribution with mean = 36 and std dev = 5, right shifted by 9
            //to guarantee the minimum steps
            int maxStep = Math.abs((int) (gen.nextGaussian() * 5 + 36)) + 9;
            //generate a random user id
            int id = gen.nextInt(9999) + 1;
            Game game = new Game(maxStep, "u"+id);
            User temp = new User("u" + id);

            //if there are already games queued up for the user, we add the game to que
            if (retVal.contains(temp)) {
                temp = retVal.get(retVal.indexOf(temp));
                temp.addGameToQue(game);
            }
            //otherwise, make a new user and add him to the queue
            else {
                temp.addGameToQue(game);
                retVal.add(temp);
            }
        }

        return retVal;
    }
}

/**
 * User class holds the UserID associated with the user and games queued up for him
 */
class User {
    private String UserID;
    private ArrayList<Game> gamesQue;

    public User(String usrnm) {
        UserID = usrnm;
        gamesQue = new ArrayList<>();
    }

    /**
     * Adds a game to the queue for the user
     * @param g game to be added
     */
    public void addGameToQue(Game g) {
        gamesQue.add(g);
    }

    /**
     * Checks whether there is a game started for the user
     * @return boolean for whether there's a game started for the user
     */
    public boolean isGameStarted() {
        return gamesQue.get(0).getGameNumber() != -1;
    }

    /**
     * Sets the game ID for the top most game
     * @param gameNum int gameID
     */
    public void setGameNumber(int gameNum) {
        gamesQue.get(0).setGameNumber(gameNum);
    }

    /**
     * Determines whether there's a game in the queue for this user
     * @return boolean
     */
    public boolean hasGameInQue() {
        return !gamesQue.isEmpty();
    }

    /**
     * Returns a JsonObject that contains the current move data
     * @return JsonObject with current move data
     */
    public JsonObject step() {
        if (hasGameInQue()) {
            JsonObject retVal = gamesQue.get(0).step();

            //If the current game is over, remove the game from the queue
            if (gamesQue.get(0).isGameOver()) {
                gamesQue.remove(0);
            }
            return retVal;
        }
        return null;
    }
}

/**
 * Game class that holds the game data and generates the JsonObject to print out
 */
class Game {
    static int maxGrid = 20;
    static int minGrid = -20;
    private int gameNumber, maxSteps;
    private String UserID;
    private boolean gameComplete;
    private boolean gameBegan;
    private int curStep;
    private int points;
    private int[] specialMove;
    private Random generator;

    /**
     * Constructor for the class that takes the maximum number of steps and the user id for the current game
     * @param mxStps int, specifies the number of maximum steps for the game
     * @param userID string, specifies the user id for this game
     */
    public Game(int mxStps, String userID) {
        maxSteps = mxStps;
        gameNumber = -1;
        UserID = userID;
        gameComplete = false;
        gameBegan = false;
        curStep = 0;
        points = 0;
        specialMove = new int[4];
        generator = new Random();
    }

    /**
     * Return Json Object that contains the result for 1 Move of this game
     * This function calls helper functions and simply assembles the result of those helper functions
     * into a json object
     * @return JsonObject with result of the current step
     */
    public JsonObject step() {
        JsonObject obj = null;
        curStep++;
        if (isGameOver()) gameComplete = true;

        //If it's the fist time running the game, print out game start
        if (!gameBegan) {
            obj = Json.createObjectBuilder()
                    .add("game", gameNumber)
                    .add("action", Json.createObjectBuilder()
                            .add("actionType", "gameStart")
                            .add("actionNumber", curStep))
                    .add("user", UserID)
                    .build();
            gameBegan = true;
        }
        //Else if it's during a step, print out the result from a step
        else if (!isGameOver()){
            String action = getAction();
            //if it's a regular move
            if (action.equals("Move")) {
                int x = getLocation();
                int y = getLocation();
                int point = getPoints(-1.5, 8.5);
                points+= point;
                obj = Json.createObjectBuilder()
                        .add("game", gameNumber)
                        .add("action", Json.createObjectBuilder()
                                .add("actionType", action)
                                .add("pointsAdded", point)
                                .add("actionNumber", curStep)
                                .add("location", Json.createObjectBuilder()
                                        .add("x", x)
                                        .add("y", y))
                                .add("points", points))
                        .add("user", UserID)
                        .build();
            }
            //if it's a special move
            else {
                String specialMove = getSpecialMoveType();
                int point = getPoints(10, 3);
                points+=point;
                obj = Json.createObjectBuilder()
                        .add("game", gameNumber)
                        .add("action", Json.createObjectBuilder()
                                .add("actionType", action)
                                .add("move", specialMove)
                                .add("pointsAdded", point)
                                .add("actionNumber", curStep)
                                .add("points", points))
                        .add("user", UserID)
                        .build();
            }
        }
        //If the step is the last step in the current game
        else if (isGameOver()) {
            obj = Json.createObjectBuilder()
                    .add("game", gameNumber)
                    .add("action", Json.createObjectBuilder()
                            .add("actionType", "GameEnd")
                            .add("actionNumber", curStep)
                            .add("points", points)
                            .add("gameStatus", points > 1 ? "Win" : "Loss"))
                    .add("user", UserID)
                    .build();
        }

        return obj;
    }

    /**
     * gets the action type with less than 5% chance of getting a special move
     * @return Move or SpecialMove
     */
    private String getAction() {
        boolean specialMove = generator.nextDouble() < .05;
        if (!specialMove || !isSpecialMoveAvailable()) {
            return "Move";
        }
        else {
            return "SpecialMove";
        }
    }

    /**
     * Checks and see if any special moves are still available
     * @return true/false
     */
    private boolean isSpecialMoveAvailable() {
        int sum = 0;
        for (int i : specialMove)
            sum +=i;
        return sum < 4;
    }

    /**
     * Return random int from normal distribution with mean = 10 and sigma = 4.5
     * @return int location from 1 to 20
     */
    private int getLocation() {
        int loc = Math.abs((int) (generator.nextGaussian() * 4.5 + 10));

        if (loc < 1)
            loc = 1;
        if (loc > 20)
            loc = 20;

        return loc;
    }

    /**
     * returns a random int generated from normal distribution with given mean and sigma
     * This is used for the score
     * @param mean average score
     * @param sigma standard deviation
     * @return int points from -20 to 20
     */
    private int getPoints(double mean, double sigma) {
        int pt = (int) (generator.nextGaussian() * sigma + mean);
        if (pt < -20)
            pt = -20;
        if (pt > 20)
            pt = 20;

        return pt;
    }


    /**
     * Determines which special move would be returned for the current special move.
     * @return String of the current special move
     */
    private String getSpecialMoveType() {
        int moveType = 3;

        double move = generator.nextDouble();
        if (move < .3) {
            moveType = 0;
        }
        else if (move < .55) {
            moveType = 1;
        }
        else if (move < .78) {
            moveType = 2;
        }

        while (specialMove[moveType] != 0) {
            moveType++;
            if (moveType == 4)
                moveType = 0;
        }

        specialMove[moveType] = 1;

        return specialMovesList[moveType];
    }

    /**
     * Checks whether is game is over by either checking the gameComplete boolean or whether we've
     * executed all the steps
     * @return boolean of when the game is over
     */
    public boolean isGameOver() { return gameComplete || curStep == maxSteps; }

    /**
     * Gets the game number for the current game
     * @return int representation of the current game number
     */
    public int getGameNumber() {
        return gameNumber;
    }

    /**
     * Sets the game number for current game
     * @param gameNumber game number for current game
     */
    public void setGameNumber(int gameNumber) {
        this.gameNumber = gameNumber;
    }

    /**
     * List of special moves available
     */
    private static final String[] specialMovesList = new String[] {"Shuffle", "Clear", "Invert", "Rotate"};
}