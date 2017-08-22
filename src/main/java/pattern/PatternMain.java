package pattern;

import pattern.visitor.*;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */
public class PatternMain {

    public static void main(String[] args) {
        ObjectStructure objectStructure = new ObjectStructure();
        objectStructure.add(new ConcreateElementNodeA());
        objectStructure.add(new ConcreateElementNodeB());

        Visitor visitor = new ConcreateVisitorA();
        objectStructure.action(visitor);
    }
}
