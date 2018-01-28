package homework;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.util.List;

public class T {
    public static void main(String[] args) {
        String s="我是可爱的小芳芳";
        List<Word> list= WordSegmenter.segWithStopWords(s);
        System.out.println(list);
    }
}
