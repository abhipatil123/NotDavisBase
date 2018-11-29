package database;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.TreeMap;
import java.util.Map.Entry;

public class TreeFunctions {
	
	public static int PAGESIZE = 512;// page size
	
	
	// keep track of parent nodes
	public static void setParent(RandomAccessFile table, int parent, int childPage, int midkey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			if (Builder.checkCapacity(table, parent)) {

				int content = (parent) * PAGESIZE;
				TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();
				if (numrecords == 0) {
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(1);
					content = content - 8;
					table.writeShort(content);
					table.writeInt(-1);
					table.writeShort(content);
					table.seek(content);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);

				} else {
					table.seek((parent - 1) * PAGESIZE + 2);
					short BuilderContentArea = table.readShort();
					BuilderContentArea = (short) (BuilderContentArea - 8);
					table.seek(BuilderContentArea);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);
					table.seek((parent - 1) * PAGESIZE + 2);
					table.writeShort(BuilderContentArea);
					for (int i = 0; i < numrecords; i++) {
						table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
						short off = table.readShort();
						table.seek(off + 4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey, BuilderContentArea);
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(numrecords++);
					table.seek((parent - 1) * PAGESIZE + 8);
					for (Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			} else {
				splitPage(table, parent);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
		
		// get the parent node
	public static int getParent(RandomAccessFile table, int page) {

			try {
				int numpages = (int) (table.length() / PAGESIZE);
				for (int i = 0; i < numpages; i++) {

					table.seek(i * PAGESIZE);
					byte pageType = table.readByte();

					if (pageType == 0x05) {
						table.seek(i * PAGESIZE + 4);
						int p = table.readInt();
						if (page == p)
							return i + 1;

						table.seek(i * PAGESIZE + 1);
						int numrecords = table.read();
						short[] offsets = new short[numrecords];

						for (int j = 0; j < numrecords; j++) {
							table.seek(i * PAGESIZE + 8 + 2 * j);
							offsets[i] = table.readShort();
							table.seek(offsets[i]);
							if (page == table.readInt())
								return j + 1;
						}
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
		
		private static void transferValue(RandomAccessFile table, int currentPage, int newPage, int midKey) {
			try {

				table.seek((currentPage) * PAGESIZE);
				byte pageType = table.readByte();
				int noOfBuilders = table.readByte();

				int mid = (int) Math.ceil(noOfBuilders / 2);

				int lower = mid - 1;
				int upper = noOfBuilders - lower;
				int content = 512;

				for (int i = mid; i <= noOfBuilders; i++) {

					table.seek(currentPage * PAGESIZE + 8 + (2 * i) - 2);
					short offset = table.readShort();
					table.seek(offset);

					int BuilderSize = table.readShort() + 6;
					content = content - BuilderSize;

					table.seek(offset);
					byte[] Builder = new byte[BuilderSize];
					table.read(Builder);

					table.seek((newPage - 1) * PAGESIZE + content);
					table.write(Builder);

					table.seek((newPage - 1) * PAGESIZE + 8 + (i - mid) * 2);
					table.writeShort((newPage - 1) * PAGESIZE + content);

				}

				table.seek((newPage - 1) * PAGESIZE + 2);
				table.writeShort((newPage - 1) * PAGESIZE + content);

				table.seek((currentPage) * PAGESIZE + 8 + (lower * 2));
				short offset = table.readShort();
				table.seek((currentPage) * PAGESIZE + 2);
				table.writeShort(offset);

				table.seek((currentPage) * PAGESIZE + 4);
				int rightpointer = table.readInt();
				table.seek((newPage - 1) * PAGESIZE + 4);
				table.writeInt(rightpointer);
				table.seek((currentPage) * PAGESIZE + 4);
				table.writeInt(newPage);

				byte Builders = (byte) lower;
				table.seek((currentPage) * PAGESIZE + 1);
				table.writeByte(Builders);
				Builders = (byte) upper;
				table.seek((newPage - 1) * PAGESIZE + 1);
				table.writeByte(Builders);

				int parent = TreeFunctions.getParent(table, currentPage + 1);
				if (parent == 0) {
					int parentpage = Builder.createPage(table);
					TreeFunctions.setParent(table, parentpage, currentPage, midKey);
					table.seek((parentpage - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else {
					if (Builder.rightPointer(table, parent, currentPage + 1)) {
						TreeFunctions.setParent(table, parent, currentPage, midKey);
						table.seek((parent - 1) * PAGESIZE + 4);
						table.writeInt(newPage);
					} else {
						TreeFunctions.setParent(table, parent, newPage, midKey);
					}
				}
			} catch (Exception e) {
				System.out.println("Error at splitLeafPage");
				e.printStackTrace();
			}
		}
		
		public static void splitLeaf(RandomAccessFile table, int currentPage) {
			int newPage = Builder.createNewPage(table);
			int midKey = splitData(table, currentPage);
			transferValue(table, currentPage, newPage, midKey);
		}
		
		// split the data
		private static int splitData(RandomAccessFile table, int pageNo) {
			int midKey = 0;
			try {
				table.seek((pageNo) * PAGESIZE);
				byte pageType = table.readByte();
				short numBuilders = table.readByte();
				short mid = (short) Math.ceil(numBuilders / 2);

				table.seek(pageNo * PAGESIZE + 8 + (2 * (mid - 1)));
				short addr = table.readShort();
				table.seek(addr);

				if (pageType == 0x0D)
					table.seek(addr + 2);
				else
					table.seek(addr + 4);
				midKey = table.readInt();
			} catch (Exception e) {
				e.printStackTrace();
			}

			return midKey;

		}
		
		// divide the pages
		private static void splitPage(RandomAccessFile table, int parent) {

			int newPage = Builder.createPage(table);
			int midKey = splitData(table, parent - 1);
			Builder.writePage(table, parent, newPage, midKey);

			try {
				table.seek((parent - 1) * PAGESIZE + 4);
				int rightpage = table.readInt();
				table.seek((newPage - 1) * PAGESIZE + 4);
				table.writeInt(rightpage);
				table.seek((parent - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		
		public static int checkOverFlow(RandomAccessFile file, int page, int payLoadsize) {
			int val = -1;

			try {
				file.seek((page) * PAGESIZE + 2);
				int content = file.readShort();
				if (content == 0)
					return PAGESIZE - payLoadsize;

				file.seek((page) * PAGESIZE + 1);
				int noOfBuilders = file.read();
				int pageHeaderSize = 8 + 2 * noOfBuilders + 2;

				file.seek((page) * PAGESIZE + 2);
				short startArea = (short) ((page + 1) * PAGESIZE - file.readShort());

				int space = startArea + pageHeaderSize;
				int spaceAvail = PAGESIZE - space;

				if (spaceAvail >= payLoadsize) {
					file.seek((page) * PAGESIZE + 2);
					short offset = file.readShort();
					return offset - payLoadsize;
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			return val;
		}
}
