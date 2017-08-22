package pattern.visitor;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */
public class ConcreateElementNodeA extends ElementNode {

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public String operationA() {
        return "ConcreteElementNodeA";
    }
}
