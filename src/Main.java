import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Main {
    static String text = "A";
    static int number = 1;
    public static void main(String[] args) {
        System.out.println("HELLO WORLD!!!");



        int[] intArr = new int[]{1,2};
        int int1 = 3;
        switch(1){
            case 1,3:System.out.println("ASDASD");
                break;
            case 2: break;
            default:
                System.out.println("C");
                break;
        }
        String a = "ab";
        String b = a.repeat(3);
        String text1 = b.replace('a','c');
        String text2 = b.replace("ab", "CD");
        String text3 = b.replaceAll("ab","AB");
        System.out.println(text1);
        System.out.println(text2);
        System.out.println(text3);
        if (!text.equals("FOO")) {
            String text = "B";
        }
        else {
            String text = "C";
        }

        System.out.println(text);

        int number = 2;
        System.out.println(number);
//        var listA = new ArrayList<String>();
//        listA.add("1");
//        listA.add("2");
//        listA.add("3");
//
//        var listB = new ArrayList<String>();
//        listB.add("a");
//        listB.add("b");
//        listA.addAll(1, listB);
//        var listC = List.copyOf(listA);
//        listC.add("d");
//        System.out.println(listC);


    }

}