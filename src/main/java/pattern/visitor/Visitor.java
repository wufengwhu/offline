package pattern.visitor;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */


/**
 * 抽象访问者
 */
public interface Visitor {
    void visit(ConcreateElementNodeA node);
    void visit(ConcreateElementNodeB node);
}
