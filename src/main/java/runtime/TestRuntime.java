package runtime;

public class TestRuntime {

	public static void main(String[] args) {
		// test.words,test
		RuntimeExec run = new RuntimeExec("test.words","test");
		run.submit();
	}
}
