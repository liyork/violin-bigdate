package com.wolf.test.hadoop.graph;

/**
 * Description:\t要在notes上输入完后再复制到txt中
 *
 * <br/> Created on 1/3/18 9:04 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class Node {

    private String id;
    private String neighbors;
    private int distance;
    private String state;

    public Node(String text) {
        String[] parts = text.split("\t");
        this.id = parts[0];
        this.neighbors = parts[1];
        if (parts[2].equalsIgnoreCase("")) {
            this.distance = -1;
        } else {
            this.distance = Integer.parseInt(parts[2]);
        }

        if (parts[3].equalsIgnoreCase("")) {
            this.state = "A";//便于使用hashcode操作
        } else {
            this.state = parts[3];
        }
    }

    public Node(String key,String text) {
        this(key + "\t" + text);
    }

    public static void main(String[] args) {
        Node node = new Node("1\t2,3,4\t0\tC");

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(String neighbors) {
        this.neighbors = neighbors;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
