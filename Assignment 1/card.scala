/*

Module Name: CSC 735 HW1
Author : Shusmoy Chowdhury
Date of Creation: 09/07/2023
Purpose: Learning Scala

Commands to run the program
-----> scalac card.scala
-----> scala main

*/

//importing necessary libraries

import scala.io.Source
import scala.io.StdIn
import java.io.PrintWriter
import java.io.File

//Creating a class for bank cards
class Card{
    var value:Long=0 // Value stores the card number as it is 13-16 digits we have declared it as Long(64 bit)    
  /*
        This function does the following
            1. returns the sum of all the digits in the odd places from right to left in the card number
  */
    def firstDigitSum():Long={
        var sum:Long=0 //intializing sum as 0
        var i:Long=value // storing the card number
        var c:Long=0 // counter to check which digits we are currently working
        while (i>0){ // checking the card number is greater than 0
            if (c%2==0){ // checking the current digit is in odd places from right to left 
                sum= sum+ (i%10) // adding the digit with the pervious sum
            }
            i=i/10 // going forward to the next digit
            c=c+1 // increasing the counter
        }
        sum // returning the total sum
    }
    /*
        This function does the following
            1. Double every second digit from right to left. 
            2. If doubling of a digit results in a two-digit number, add up the two digits to get a single-digit number
            3. return the total of all single digits after doubling
    */
    def secondDigitSum():Long={
        var sum:Long=0 //intializing sum as 0
        var i:Long=value // storing the card number
        var c:Int=0 // counter to check which digits we are currently working
        while (i>0){ // checking the card number is greater than 0
            if (c%2==1){ // checking the current digit is second digit from right to left
            var x:Long= (i%10)*2 // double the current digit
            var y:Long= (x%10) + (x/10)%10 //f doubling of a digit results in a two-digit number, add up the two digits to get a single-digit number
            sum = sum + y // adding the single digit with the pervious sum
            }
            i=i/10 // going forward to the next digit
            c=c+1 // increasing the counter
        }
        sum // returning the total sum
    }
    /*
        This function does the following
            1. checks the validity of the bank card
            2. returns a string either valid or invalid
    */
    def validityCheck():String={
        val digitSum1:Long=firstDigitSum() // calculates and stores the first digit sum
        val digitSum2:Long=secondDigitSum() //  calculates and stores the second digit sum
        var validity:String="" // variable to return the validity
        if ((digitSum1+digitSum2)%10==0) // checks if the sum of firstDigitSum and secondDigitSum is divisible to sum
        {
            println("valid") // if the sum is divisible to 10 then card is valid so printing valid
            validity="valid\n" // storing the valid in the variable
        }
        else{
            println("invalid") // if the sum is divisible to 10 then card is valid so printing valid
            validity="invalid\n" // storing the invalid in the variable
        }
        validity // returning the validity of the card
    }
    
}
/*
    
    main function for the program

*/

object main extends App{
    val bufferedSource = Source.fromFile("number.txt") // reading the input file
    val fileWriter = new PrintWriter(new File("output.txt"))// opening output file to write the output   
    val c:Card= new Card // creating card object
    for (line <- bufferedSource.getLines) { // reading each line from the file
        c.value=line.toLong // storing the card value in the object
        val s:String= c.value.toString() + "------->"+ c.validityCheck() // card object and its validity to write on output.txt
        fileWriter.write(s) // checking the card validity, printing it in console and writing in the file
    }
    bufferedSource.close() // closing input file
    fileWriter.close() // closing output file
}