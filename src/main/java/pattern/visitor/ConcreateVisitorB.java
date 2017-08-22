package pattern.visitor;


import pattern.visitor.ConcreateElementNodeA;
import pattern.visitor.ConcreateElementNodeB;
import pattern.visitor.Visitor;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */
public class ConcreateVisitorB implements Visitor {


    @Override
    public void visit(ConcreateElementNodeA node) {
        System.out.println(node.operationA());
    }

    @Override
    public void visit(ConcreateElementNodeB node) {
        System.out.println(node.operationB());

    }
}
