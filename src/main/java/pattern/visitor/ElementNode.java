package pattern.visitor;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */
abstract class ElementNode {
    public abstract void accept(Visitor visitor);
}
