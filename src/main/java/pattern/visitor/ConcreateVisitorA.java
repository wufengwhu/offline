package pattern.visitor;


/**
 * Created by fengwu
 * Date: 2017/8/21
 * <p>
 * 具体访问者
 */
public class ConcreateVisitorA implements Visitor {

    @Override
    public void visit(ConcreateElementNodeA node) {
        System.out.println(node.operationA());

    }

    @Override
    public void visit(ConcreateElementNodeB node) {
        System.out.println(node.operationB());

    }
}
