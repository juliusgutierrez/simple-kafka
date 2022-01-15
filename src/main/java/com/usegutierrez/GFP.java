package com.usegutierrez;

import java.util.ArrayList;
import java.util.List;

class GFG {

  // instance function to be called
  void someFunction(String s) {
    System.out.println(s);
  }

  public static void main(String[] args) {

    List<String> list = new ArrayList<String>();
    list.add("Geeks");
    list.add("For");
    list.add("GEEKS");

    // call the instance method
    // using double colon operator
    list.forEach((new GFG())::someFunction);
  }
}